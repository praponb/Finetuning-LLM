from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        F.date_trunc("month", "opened_date").alias("opened_date_period"),
    )
)
