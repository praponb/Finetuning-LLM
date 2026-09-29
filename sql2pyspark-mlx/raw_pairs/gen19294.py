from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", "s.city", "s.rating", F.col("c.category"))
)

result = (
    with_last
    .select(
        "id",
        "country",
        "rating",
        "category",
        F.when(F.col("category").rlike("^(Books|Home)$"), "GROUP_A").otherwise("GROUP_B").alias("category_zone"),
    )
    .orderBy("category")
)
