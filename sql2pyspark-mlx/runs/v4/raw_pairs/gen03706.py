from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())

deduped = (
    df_suppliers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        "row_num",
        F.when(F.col("city").rlike("^(London|Berlin)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
)
