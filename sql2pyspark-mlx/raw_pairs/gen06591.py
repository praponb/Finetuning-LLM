from pyspark.sql import functions as F

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-07-15").cast("date")) & (F.col("region") == "North"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.coalesce("sqft", F.lit(0)).alias("sqft_filled"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)
