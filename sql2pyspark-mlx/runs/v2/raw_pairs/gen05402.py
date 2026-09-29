from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.filter(F.col("channel").isin(["store", "partner", "mobile"])).alias("o"), F.col("s.id") == F.col("o.store_id"), "left_semi")
    .select(
        F.col("s.id"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("sqft").desc(), F.col("id").desc())
    .limit(3)
)
