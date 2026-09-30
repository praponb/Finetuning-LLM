from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.revenue") < 3400), "left_anti")
)

result = (
    not_flagged
    .select("id", "category", "brand", "price", "cost", "launch_date", "tags")
    .orderBy(F.col("category").asc_nulls_last(), F.col("id").desc())
)
