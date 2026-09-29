from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(4592, 14906))
    .select(
        "store_name",
        "opened_date",
        "region",
        F.length("region").alias("region_length"),
    )
    .orderBy(F.col("region").desc())
)
