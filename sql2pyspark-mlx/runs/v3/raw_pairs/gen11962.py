from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isNull() & ~F.col("region").isin(["South", "Central"]))
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.concat("region", F.lit("/"), "store_name").alias("region_store_name"),
    )
    .orderBy(F.col("id").desc())
)
