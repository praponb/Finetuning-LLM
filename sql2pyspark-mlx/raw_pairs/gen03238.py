from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("ship_country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w_part = Window.partitionBy(F.month("created_at"), "ship_country").orderBy("bulk_lines", "id").rowsBetween(-5, Window.currentRow)

unpacked = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.coalesce(F.col("attributes").getItem("utm_source"), F.lit("none")).alias("utm_source_or_default"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "bulk_lines",
        "utm_source_or_default",
        F.count("bulk_lines").over(w).alias("bulk_lines_count_30m"),
        F.avg("bulk_lines").over(w_part).alias("bulk_lines_avg_5"),
    )
)
