from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code")

exploded = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "li_item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("li_item.qty"),
        F.col("li_item.price"),
        F.col("li_item.promo"),
    )
)

lines = exploded.select("id", "line_no", "qty", F.explode("coupon_codes").alias("code"))

enriched = (
    lines
    .filter(F.col("code") == "BUNDLE")
    .withColumn("code_avg_line_no", F.avg("line_no").over(w))
)

valid = (
    enriched
    .filter((~F.col("code").eqNullSafe(F.lit("SPRING10"))) & (F.col("line_no") == 2))
    .select("*", F.round((F.col("qty") * 0.9), 2).alias("qty_scaled"))
)

result = (
    valid
    .groupBy("code")
    .agg(
        F.countDistinct("code").alias("distinct_codes"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.round(F.sum("code_avg_line_no") / F.nullif(F.count(F.when(F.col("code") == "BUNDLE", 1)), F.lit(0)), 2).alias("code_avg_line_no_per_bundle"),
        F.count("*").alias("cnt_2"),
    )
)
