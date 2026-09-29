from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.filter(F.col("sqft").between(10129, 11882)).alias("s"), F.col("ss.store_id") == F.col("s.id"), "left_semi")
    .filter(F.col("ss.product_id").isin([5, 9]))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
    )
)
