from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w_running = Window.orderBy("category_length", "id")
w3 = Window.partitionBy("brand", "supplier_id").orderBy(F.col("category_length").asc_nulls_last(), "id")

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("margin", (F.col("price") * F.col("cost")))
    .withColumn("least_stock_qty_price", F.least("stock_qty", "price"))
)

valid = (
    filtered
    .filter((F.col("launch_date") >= F.lit("2023-09-02").cast("date")) & (F.col("launch_date") >= "2024-09-01"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "margin",
        F.length("category").alias("category_length"),
    )
)

scored = (
    valid
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("next_stock_qty", F.lead("stock_qty", 1).over(w_running))
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("category") == "Books", F.col("price")).otherwise(0)).alias("books_price"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .filter(F.col("books_price") >= 0)
)
