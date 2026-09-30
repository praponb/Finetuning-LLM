from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("supplier_id", "category")
w3 = Window.orderBy("cost_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w_hist = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("brand").orderBy("cost_avg_2", "id")
w8 = Window.partitionBy("supplier_id", "brand").orderBy("cost_avg_2_bucket", "id").rowsBetween(-6, -1)
w9 = Window.partitionBy("supplier_id").orderBy("running_count_next_event_seq", "id")
w10 = Window.partitionBy("brand").orderBy("running_max_next_event_seq", "id")
w11 = Window.partitionBy("supplier_id").orderBy("next_first_known_next_stock_qty_minus_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("supplier_id").orderBy("prev_stock_qty_minus_price", "id")
w13 = Window.orderBy(F.col("price").desc(), "id")

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.upper("category").alias("category_upper"),
    )
)

scored = (
    valid
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        "category_upper",
        F.lag("cost").over(w_grp).alias("prev_cost"),
        F.lag("stock_qty_minus_price").over(w_grp).alias("prev_stock_qty_minus_price"),
        F.lead("stock_qty_minus_price").over(w_grp).alias("next_stock_qty_minus_price"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "tags",
        "supplier_id",
        "prev_stock_qty_minus_price",
        "next_stock_qty_minus_price",
        F.first("next_stock_qty_minus_price", ignorenulls=True).over(w3).alias("first_known_next_stock_qty_minus_price"),
        F.avg("cost").over(w4).alias("cost_avg_2"),
        F.lead("event_seq", 2).over(w5).alias("next_event_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.avg("next_event_seq").over(w_hist).alias("running_avg_next_event_seq"),
        F.count("next_event_seq").over(w_hist).alias("running_count_next_event_seq"),
        F.max("next_event_seq").over(w_hist).alias("running_max_next_event_seq"),
        F.ntile(10).over(w7).alias("cost_avg_2_bucket"),
    )
)

enriched = (
    metrics
    .filter(F.col("running_count_next_event_seq") < 7)
    .withColumn("running_max_next_event_seq_avg_6_prior", F.avg("running_max_next_event_seq").over(w8))
    .withColumn("running_count_next_event_seq_bucket", F.ntile(5).over(w9))
    .withColumn("next_first_known_next_stock_qty_minus_price", F.lead("first_known_next_stock_qty_minus_price", 1).over(w10))
)

result = (
    enriched
    .select(
        "id",
        "prev_stock_qty_minus_price",
        "cost_avg_2",
        "running_avg_next_event_seq",
        "cost_avg_2_bucket",
        "running_count_next_event_seq_bucket",
        "next_first_known_next_stock_qty_minus_price",
        F.first("cost_avg_2", ignorenulls=True).over(w11).alias("first_known_cost_avg_2"),
        F.ntile(3).over(w12).alias("prev_stock_qty_minus_price_bucket"),
        F.row_number().over(w13).alias("row_seq"),
    )
)
