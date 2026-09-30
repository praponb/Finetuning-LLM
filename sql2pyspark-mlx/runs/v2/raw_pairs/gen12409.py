from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").like("%Flagshi%") & (F.col("opened_date") <= "2024-11-21")) | F.col("region").isin(["North", "South", "West"]))
    .select(
        "sqft",
        "region",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
    )
)
