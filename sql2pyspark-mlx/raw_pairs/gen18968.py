from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock_plus_months", "id")
w2 = Window.partitionBy("store_id").orderBy("rank_in_group", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("rank_in_group", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("prev_quantity_rounded").desc_nulls_last())
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("units").alias("sum_store_sale_units"))

base = (
    df_inventory
    .filter(F.col("product_id").isin([4, 6, 8]))
    .select("*", F.round("quantity", 2).alias("quantity_rounded"))
)

valid = (
    base
    .filter(F.col("last_restock") > F.lit("2023-03-02").cast("date"))
    .select("*", F.add_months("last_restock", 12).alias("last_restock_plus_months"))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .filter(F.col("rank_in_group") <= 1)
    .select(
        "id",
        "store_id",
        "quantity_rounded",
        "rank_in_group",
        F.lead("quantity_rounded").over(w2).alias("next_quantity_rounded"),
        F.lag("quantity_rounded").over(w2).alias("prev_quantity_rounded"),
        F.lag("quantity").over(w2).alias("prev_quantity"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

base_inventory = (
    scored
    .filter(F.col("rank_in_group") > 1)
    .withColumn("seq_no_minus_row_seq", (F.col("seq_no") - F.col("row_seq")))
    .withColumn("next_quantity_rounded_times_prev_quantity", (F.col("next_quantity_rounded") * F.col("prev_quantity")))
)

ranked = (
    base_inventory
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

valid_inventory = (
    ranked
    .filter(F.col("prev_quantity_rounded") >= 52)
    .select(
        "id",
        "store_id",
        "quantity_rounded",
        "rank_in_group",
        "next_quantity_rounded",
        "prev_quantity_rounded",
        "row_seq",
        "seq_no",
        "seq_no_minus_row_seq",
        "next_quantity_rounded_times_prev_quantity",
        "dedup_rank",
        (F.col("next_quantity_rounded") + F.col("seq_no")).alias("next_quantity_rounded_plus_seq_no"),
        F.col("rank_in_group").cast("double").alias("rank_in_group_str"),
    )
)

result = (
    valid_inventory
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "quantity_rounded",
        "rank_in_group",
        "next_quantity_rounded",
        "row_seq",
        "seq_no",
        "seq_no_minus_row_seq",
        "next_quantity_rounded_times_prev_quantity",
        F.col("sum_store_sale_units"),
    )
    .orderBy(F.col("seq_no").desc_nulls_first())
)
