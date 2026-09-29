from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w_grp = Window.partitionBy("brand")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")
w5 = Window.orderBy("cost_share", "id")
w6 = Window.orderBy("cost_share", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.orderBy("prev_seq_no", "id")
w8 = Window.orderBy(F.col("seq_no_avg_2").desc())
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-07-22").cast("date")) & F.col("brand").isNotNull())
    .withColumn("is_grocery", F.when(F.col("category") == "Grocery", 1).otherwise(0))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("N/A")))
)

metrics = (
    valid
    .filter(F.col("brand").isin(["Globex", "Umbrella"]))
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.count("is_grocery").over(w_grp).alias("brand_count_is_grocery"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "cost_share",
        "seq_no",
        "brand_count_is_grocery",
    )
)

scored = (
    latest
    .select(
        "id",
        "launch_date",
        F.lead("seq_no").over(w5).alias("next_seq_no"),
        F.lag("seq_no").over(w5).alias("prev_seq_no"),
        F.lag("cost_share").over(w5).alias("prev_cost_share"),
        F.lead("cost_share").over(w5).alias("next_cost_share"),
        F.row_number().over(w5).alias("row_seq"),
        F.avg("seq_no").over(w6).alias("seq_no_avg_2"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.lead("next_cost_share", 2).over(w_part).alias("next_next_cost_share"),
        F.lag("prev_seq_no", 1, 0).over(w_part).alias("prev_prev_seq_no"),
        F.cume_dist().over(w8).alias("seq_no_avg_2_cume_dist"),
    )
)

valid_products = (
    with_windows
    .filter(F.col("launch_date") >= F.lit("2023-08-08").cast("date"))
    .select(
        "id",
        "launch_date",
        "prev_seq_no",
        "prev_cost_share",
        "next_cost_share",
        "row_seq",
        "seq_no_avg_2",
        "prev_prev_seq_no",
        "seq_no_avg_2_cume_dist",
        F.col("seq_no_avg_2").cast("double").alias("seq_no_avg_2_str"),
    )
)

latest_match = (
    valid_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

result = (
    latest_match
    .select(
        "id",
        "prev_seq_no",
        "prev_cost_share",
        "next_cost_share",
        "row_seq",
        "seq_no_avg_2",
        "prev_prev_seq_no",
        "seq_no_avg_2_cume_dist",
        "seq_no_avg_2_str",
    )
    .orderBy(F.col("prev_seq_no").asc_nulls_first())
)
