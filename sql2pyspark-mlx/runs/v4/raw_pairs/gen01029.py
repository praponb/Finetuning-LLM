from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("brand").orderBy(F.col("cost").desc_nulls_first())

ranked = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_products = (
    ranked
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

kept_keys = (
    ranked_products
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.col("category").isin(["Fashion", "Electronics"]))
        .select("supplier_id")
    )
)

reconciled = (
    ranked_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.year(F.col("ch.last_restock")) == 2023), "left_anti")
)

result = (
    not_flagged
    .select("brand", F.col("cost").alias("COST"), F.col("price").alias("PRICE_VAL"))
    .unpivot(["brand"], ["COST", "PRICE_VAL"], "kpi", "value")
    .filter(F.col("value").isNotNull())
)
