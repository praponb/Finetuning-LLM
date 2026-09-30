from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "*",
        F.cume_dist().over(w).alias("discount_pct_cume_dist"),
        F.row_number().over(w2).alias("row_seq"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
    )
)
