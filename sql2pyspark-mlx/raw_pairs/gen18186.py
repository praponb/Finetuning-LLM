from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy("num_attributes", "id")

unpacked = (
    df_carts
    .filter(F.to_date("created_at") >= "2025-01-14")
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.size("attributes").alias("num_attributes"),
        F.aggregate("items", F.lit(0.0), lambda acc, li: acc + li["qty"] * li["price"], lambda acc: F.round(acc, 2)).alias("cart_value"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
    )
)

result = (
    unpacked
    .filter(F.hour("created_at").between(6, 13))
    .select(
        "id",
        "ship_country",
        "num_attributes",
        "cart_value",
        F.lead("cart_value").over(w).alias("next_cart_value"),
        F.lag("cart_value").over(w).alias("prev_cart_value"),
        F.lead("num_attributes").over(w).alias("next_num_attributes"),
        F.row_number().over(w).alias("row_seq"),
    )
)
