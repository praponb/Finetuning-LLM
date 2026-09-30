from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy(F.col("weight_kg").desc(), "id")
w2 = Window.orderBy(F.col("weight_kg").desc_nulls_last())

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "items",
        "ship_to",
        F.coalesce(F.col("ship_to.country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    parsed
    .select(
        "*",
        F.ntile(3).over(w_running).alias("weight_kg_bucket"),
        F.cume_dist().over(w2).alias("weight_kg_cume_dist"),
    )
)
