from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.quarter("opened_date").alias("opened_quarter"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(F.max("sqft").alias("max_sqft"))
    .orderBy("opened_year")
)
