from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc(), "id")
w2 = Window.partitionBy("promo_code").orderBy("ends_on", "id")
w3 = Window.partitionBy(F.date_trunc("week", "starts_on"), "promo_code").orderBy("discount_pct", "id")
w4 = Window.orderBy("ends_on", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_promotions
    .filter(F.year("starts_on") == 2023)
    .select(
        "id",
        "channel",
        "discount_pct",
        F.ntile(4).over(w).alias("discount_pct_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("discount_pct").over(w4).alias("discount_pct_avg_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "channel",
        "discount_pct_bucket",
        "seq_no",
        "row_seq",
        "discount_pct_avg_2",
        F.when(F.col("channel").rlike("^(web|mobile)$"), "CORE").otherwise("EXTENDED").alias("channel_zone"),
    )
)
