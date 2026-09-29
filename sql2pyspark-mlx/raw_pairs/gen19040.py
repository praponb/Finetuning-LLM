from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date") <= "2025-10-01")
    .select(
        "id",
        "store_name",
        "opened_date",
        "region",
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)
