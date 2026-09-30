from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc_nulls_first())

deduped = (
    df_suppliers
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        "rn",
        F.when(F.col("supplier_name").rlike("^(Tokyo Traders|Pacific Wholesale)$"), "GROUP_A").when(F.col("supplier_name").rlike("^(Siam Parts Co|Global Supply)$"), "PRIORITY").otherwise("GROUP_B").alias("supplier_name_zone"),
    )
    .orderBy(F.col("supplier_name_zone").desc_nulls_first())
)
