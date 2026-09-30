from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-21"))) & F.col("region").isNotNull())
    .select("*", F.trim("store_name").alias("clean_store_name"))
)

result = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left_semi")
)
