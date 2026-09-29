from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.col("s.opened_date") >= "2023-04-01") & F.col("s.store_name").isNotNull())
    .select(
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("ss.sale_date"),
        F.col("ss.units"),
    )
    .orderBy("store_name", "sale_date")
)
