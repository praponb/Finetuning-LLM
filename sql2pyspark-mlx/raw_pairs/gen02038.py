from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("weight_kg").desc())
w_part = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["gift"].alias("gift"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.array_join(F.array_sort("coupon_codes"), ",").alias("coupons_text"),
        F.size("attributes").alias("num_attributes"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "coupons_text",
        F.percent_rank().over(w).alias("weight_kg_percent_rank"),
        F.max("num_attributes").over(w_part).alias("num_attributes_max_2h"),
        F.count("weight_kg").over(w_part).alias("weight_kg_count_2h"),
    )
)
