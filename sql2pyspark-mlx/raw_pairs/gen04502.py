from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("opened_date").between("2023-10-22", "2023-12-01"))
    .select(
        "id",
        "store_name",
        "opened_date",
        "sqft",
        F.concat_ws("-", "region", "city").alias("region_city_key"),
    )
)

result = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.status") == "PENDING"), "left_anti")
)
