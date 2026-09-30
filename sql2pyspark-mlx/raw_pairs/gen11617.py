from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.to_date("created_at") >= "2023-04-18")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        F.aggregate("items", F.lit(0.0), lambda acc, item: acc + item["qty"] * item["price"], lambda acc: F.round(acc, 2)).alias("basket_amount"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
        F.col("attributes")["device"].alias("device"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        "customer_id",
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("basket_amount") >= 1300, "PREMIUM").otherwise("STANDARD").alias("basket_amount_tier"),
    )
    .agg(
        F.sum("basket_amount").alias("total_basket_amount"),
        F.max("created_at").alias("last_created_at"),
        F.min("created_at").alias("first_seen"),
        F.round(F.avg("basket_amount"), 2).alias("avg_basket_amount"),
        F.sum("basket_amount").alias("total_basket_amount_2"),
    )
    .filter(F.col("avg_basket_amount") > 0)
    .orderBy(F.col("total_basket_amount_2").desc())
)
