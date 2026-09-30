from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_last())

latest = (
    df_suppliers
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "city",
        "rating",
        "rn",
        F.when(F.col("city").rlike("^(Bangkok|Tokyo)$"), "CORE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
)
