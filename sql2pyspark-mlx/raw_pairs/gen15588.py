from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id")

enriched = (
    df_store_sales
    .filter(F.col("units").between(32, 37))
    .withColumn("prev_units", F.lag("units").over(w))
    .withColumn("next_units", F.lead("units").over(w))
    .withColumn("prev_revenue", F.lag("revenue").over(w))
    .withColumn("next_revenue", F.lead("revenue").over(w))
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w2)), 4))
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "prev_units",
        "prev_revenue",
        "next_revenue",
        "event_seq",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
