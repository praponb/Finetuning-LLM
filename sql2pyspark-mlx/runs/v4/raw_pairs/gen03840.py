from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "attributes",
        F.col("attributes").getItem("device").alias("device"),
        F.transform(
            F.filter("items", lambda item: item["promo"].isNotNull()),
            lambda item: F.struct(item["product_id"].alias("pid"), F.round(item["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.size("attributes").alias("num_attributes"),
    )
)
