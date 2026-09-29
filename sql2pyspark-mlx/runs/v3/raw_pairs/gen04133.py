from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "store_name",
        "sqft",
        "region",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
    )
)
