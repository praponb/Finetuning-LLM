from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code")

base = (
    df_promotions
    .filter(F.col("ends_on").between("2023-08-01", "2025-06-01"))
    .withColumn("ends_on_month_end", F.last_day("ends_on"))
)

result = (
    base
    .select(
        "*",
        F.avg("discount_pct").over(w).alias("discount_pct_avg_5"),
        F.row_number().over(w2).alias("event_seq"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4).alias("discount_pct_share"),
    )
)
