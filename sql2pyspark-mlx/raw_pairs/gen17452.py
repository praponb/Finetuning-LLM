from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel")
w2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("ends_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        F.round((F.col("discount_pct") / F.sum("discount_pct").over(w)), 4).alias("discount_pct_share"),
        F.first("channel", ignorenulls=True).over(w2).alias("first_known_channel"),
        F.sum("discount_pct").over(w3).alias("running_discount_pct"),
        F.last("channel", ignorenulls=True).over(w3).alias("last_known_channel"),
    )
)
