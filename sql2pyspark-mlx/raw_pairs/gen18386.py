from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("city").isin(["Bangkok", "Tokyo"]), "priority").otherwise("other").alias("city_group"),
        "city",
    )
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("total_sqft") < 78000)
)
