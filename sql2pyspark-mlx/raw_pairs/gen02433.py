from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code", "channel").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w4 = Window.partitionBy("promo_code")

result = (
    df_promotions
    .filter(F.col("ends_on").between("2023-08-01", "2025-05-01"))
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.row_number().over(w).alias("row_seq"),
        F.sum("discount_pct").over(w2).alias("running_discount_pct"),
        F.lag("discount_pct", 1).over(w3).alias("prev_discount_pct"),
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w4)), 4).alias("discount_pct_share"),
    )
)
