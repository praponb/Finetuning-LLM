from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("opened_date") > "2023-12-01") & F.col("city").isin(["New York", "Bangkok"]) & F.col("sqft").isNull()))
    .select(
        "opened_date",
        "region",
        "store_name",
        F.substring("store_name", 1, 2).alias("store_name_prefix"),
        F.date_sub("opened_date", 25).alias("opened_date_minus_25d"),
    )
)
