from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy(F.col("id").desc(), F.col("sqft").desc())
    .limit(10)
)
