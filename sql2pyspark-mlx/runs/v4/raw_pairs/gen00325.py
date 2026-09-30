from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel").orderBy("discount_pct")
w_part = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

result = (
    df_promotions
    .select(
        "*",
        F.first("channel", ignorenulls=True).over(w).alias("first_known_channel"),
        F.cume_dist().over(w2).alias("discount_pct_cume_dist"),
        F.dense_rank().over(w_part).alias("discount_pct_dense_rank"),
    )
)
