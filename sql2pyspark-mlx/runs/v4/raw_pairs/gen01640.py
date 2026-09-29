from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .groupBy(F.quarter(F.col("s.opened_date")).alias("opened_quarter"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
        F.max(F.col("ss.units")).alias("max_units"),
    )
    .orderBy(F.col("max_units").desc(), "opened_quarter")
    .limit(50)
)
