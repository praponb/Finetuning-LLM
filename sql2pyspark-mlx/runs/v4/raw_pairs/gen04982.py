from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2024-02-24").cast("date")) & F.col("brand").isNotNull() & F.col("category").isin(["Home", "Grocery"]))
    .withColumn("price_filled", F.coalesce("price", F.lit(0)))
    .withColumn("launch_date_period", F.date_trunc("WEEK", "launch_date"))
)

result = (
    base
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_anti")
)
