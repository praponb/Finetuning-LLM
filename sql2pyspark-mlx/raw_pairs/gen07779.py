from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")

result = (
    df_promotions
    .select(
        "id",
        "promo_code",
        "ends_on",
        F.lead("discount_pct").over(w).alias("next_discount_pct"),
        F.row_number().over(w).alias("seq_no"),
    )
)
