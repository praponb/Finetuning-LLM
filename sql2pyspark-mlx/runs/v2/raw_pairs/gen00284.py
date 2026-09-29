from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "sqft",
        "region",
        F.date_add("opened_date", 15).alias("opened_date_plus_15d"),
    )
    .orderBy("opened_date_plus_15d", F.col("sqft").desc())
)
