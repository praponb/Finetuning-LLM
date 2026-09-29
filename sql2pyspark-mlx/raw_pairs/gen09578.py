from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["London", "New York", "Bangkok"]))
    .select(
        "id",
        "sqft",
        "region",
        F.date_add("opened_date", 48).alias("opened_date_plus_48d"),
    )
)
