from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("rn").desc())
w3 = Window.partitionBy("supplier_id").orderBy("cost", "id")
w4 = Window.orderBy(F.col("launch_date").desc(), "id")
w5 = Window.orderBy("stock_qty", "id")
w6 = Window.orderBy(F.col("rn").desc(), "id")
w7 = Window.partitionBy("category").orderBy(F.col("num_categories").desc())

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-08-17")))
    .select("*", F.round("cost", 2).alias("cost_rounded"))
)

ranked = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .filter(F.col("cost_rounded") >= 30)
    .select(
        "*",
        F.lag("cost", 2).over(w3).alias("prev_cost"),
        F.row_number().over(w4).alias("row_seq"),
        F.ntile(3).over(w5).alias("stock_qty_bucket"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

result = (
    scored
    .groupBy("category", F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_rows"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .withColumn("num_categories_percent_rank", F.percent_rank().over(w7))
    .orderBy(F.col("stock_qty_stddev").asc_nulls_first())
)
