from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo_code", "channel").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .filter(F.col("starts_on").between("2023-09-07", "2024-03-01"))
    .select(
        "*",
        F.min("discount_pct").over(w_ordered).alias("running_min_discount_pct"),
        F.count("discount_pct").over(w_ordered).alias("running_count_discount_pct"),
        F.sum("discount_pct").over(w_ordered).alias("running_sum_discount_pct"),
    )
)
