from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .select("id", "country", "rating", "supplier_name")
    .orderBy(F.col("rating").desc())
)
