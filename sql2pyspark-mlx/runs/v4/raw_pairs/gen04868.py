from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("price", "id").rowsBetween(-6, -1)
w_part = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_products
    .select(
        "*",
        F.avg("price").over(w).alias("price_avg_6_prior"),
        F.sum("price").over(w_part).alias("running_price"),
        F.first("brand", ignorenulls=True).over(w3).alias("first_known_brand"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("price_avg_6_prior") >= 1110, "PREMIUM").otherwise("STANDARD").alias("price_avg_6_prior_tier"),
        "supplier_id",
    )
    .agg(
        F.percentile_approx("running_price", 0.9).alias("running_price_p90"),
        F.round(F.sum("price_avg_6_prior") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("price_avg_6_prior_per_globex"),
    )
    .filter(F.col("price_avg_6_prior_per_globex") < 0)
    .orderBy("price_avg_6_prior_per_globex")
)
