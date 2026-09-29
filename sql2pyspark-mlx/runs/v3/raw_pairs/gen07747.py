from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("city") == "Singapore") & (F.col("region") != "Central")) | F.col("region").isin(["South", "West"]))
    .select(
        "id",
        "opened_date",
        "region",
        "store_name",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
    .orderBy("region")
)
