from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city") != "New York")
    .select(
        "id",
        "region",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
    )
)
