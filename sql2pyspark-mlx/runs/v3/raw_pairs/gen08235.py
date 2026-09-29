from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "West") | F.col("region").isin(["West", "Central"]))
    .select(
        "id",
        "opened_date",
        F.when(F.col("city").isin(["London", "Tokyo"]), "Priority").otherwise("Other").alias("city_group"),
    )
)
