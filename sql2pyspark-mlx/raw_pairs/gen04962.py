from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.when(F.col("city").isin(["London", "New York"]), "priority").otherwise("other").alias("city_group"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.max("sqft").alias("max_sqft"),
    )
    .orderBy("city_group")
)
