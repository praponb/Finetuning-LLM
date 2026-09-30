from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

enriched = (
    df_suppliers
    .select(
        "*",
        F.lead("rating").over(w).alias("next_rating"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "event_seq",
        F.when(F.col("country").rlike("^(TH|DE)$"), "PRIORITY").when(F.col("country").rlike("^(US|SG)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("country_zone"),
    )
    .orderBy(F.col("rating").desc_nulls_first())
)
