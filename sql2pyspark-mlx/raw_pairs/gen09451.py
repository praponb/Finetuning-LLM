from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") >= 9151) & (F.col("sqft") > 8726) & (F.col("sqft") == 10315))
    .select(
        "id",
        "opened_date",
        "region",
        "city",
        F.upper("region").alias("region_upper"),
    )
    .orderBy("city")
)
