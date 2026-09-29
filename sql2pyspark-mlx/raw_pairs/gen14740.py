from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["qty"] * x["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)
