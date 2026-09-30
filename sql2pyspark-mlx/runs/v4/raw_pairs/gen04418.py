from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("discount_pct", "id")

scored = (
    df_promotions
    .select(
        "*",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.ntile(3).over(w2).alias("discount_pct_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "channel",
        "starts_on",
        "discount_pct",
        "cumulative_discount_pct",
        "discount_pct_bucket",
        F.when(F.col("channel").rlike("^(partner|web)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(mobile|store)$"), "GROUP_B").otherwise("CORE").alias("channel_zone"),
    )
    .orderBy("cumulative_discount_pct")
)
