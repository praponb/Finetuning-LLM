from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").between("2024-03-01", "2024-05-13") | F.col("region").isin(["Central", "East"]))
    .select(
        "id",
        "sqft",
        "opened_date",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)
