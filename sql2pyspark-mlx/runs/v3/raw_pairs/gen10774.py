from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "opened_date",
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
    )
    .orderBy(F.col("sqft").desc(), "opened_date_period")
)
