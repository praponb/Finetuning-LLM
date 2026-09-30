from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-6, Window.currentRow)

enriched = (
    df_stores
    .filter(F.col("city").isin(["New York", "Chiang Mai"]))
    .withColumn("prev_sqft", F.lag("sqft").over(w))
    .withColumn("next_sqft", F.lead("sqft").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "row_seq",
        "sqft_avg_6",
        F.when(F.col("store_name").rlike("^(Downtown Express|Old Town)$"), "OTHER_ZONE").when(F.col("store_name").rlike("^(Siam Flagship|Airport Kiosk)$"), "GROUP_B").otherwise("CORE").alias("store_name_zone"),
    )
    .orderBy(F.col("id").desc(), F.col("store_name_zone").desc())
    .limit(5)
)
