from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on", "id")

result = (
    df_promotions
    .filter(F.col("ends_on") > "2023-10-02")
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.sum("discount_pct").over(w_part).alias("cumulative_discount_pct"),
        F.lead("discount_pct", 2).over(w2).alias("next_discount_pct"),
    )
)
