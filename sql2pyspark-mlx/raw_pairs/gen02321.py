from pyspark.sql import functions as F

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").isNull())
    .select(
        "id",
        "city",
        "region",
        "sqft",
        F.quarter("opened_date").alias("opened_date_quarter"),
    )
)

result = (
    filtered
    .select("id", "region")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["West", "Central", "East"]))
        .select("id", "region")
    )
)
