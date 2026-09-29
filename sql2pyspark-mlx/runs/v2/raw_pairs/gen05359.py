from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("row_num").desc(), "supplier_name")
)
