from pyspark.sql import functions as F

lines = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    lines
    .select(
        "id",
        "customer_id",
        "line_no",
        "qty",
        "price",
        "promo",
        F.when(F.col("promo").rlike("^(VIP|BUNDLE)$"), "GROUP_B").otherwise("PRIORITY").alias("promo_zone"),
    )
    .orderBy(F.col("price").asc_nulls_last())
)
