from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2025-08-27", "2025-10-01"))
    .select(
        "id",
        "region",
        "opened_date",
        F.round("sqft", 0).alias("sqft_rounded"),
        F.month("opened_date").alias("opened_date_month"),
    )
    .orderBy("opened_date_month")
)
