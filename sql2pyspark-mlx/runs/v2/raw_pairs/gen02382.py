from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Sia%"))
    .select(
        "opened_date",
        "region",
        "city",
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
    )
    .orderBy("region")
)
