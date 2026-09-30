from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") == "Berlin") | (F.col("opened_date") > "2025-08-01")))
    .select(
        "id",
        "store_name",
        F.round((F.col("sqft") * 1.1), 2).alias("adjusted_sqft"),
        F.date_sub("opened_date", 42).alias("opened_date_minus_42d"),
    )
    .orderBy(F.col("adjusted_sqft").desc())
)
