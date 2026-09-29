from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_store_sales.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .filter((F.year(F.col("s.opened_date")) >= 2023) & F.col("ss.product_id").isin([3, 4, 9, 10]))
    .select(
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.region"),
        F.col("ss.units"),
    )
)
